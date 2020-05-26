# frozen_string_literal: true

# class Big::Renda
class Abank::Big::Contrato::Rendas < Abank::Big::Contrato
  attr_reader :ren

  # @return [Integer] ano renda
  attr_reader :ran
  # @return [Integer] mes renda
  attr_reader :rcn
  # @return [Float] valor renda mensal
  attr_reader :rru

  # @return [Integer] total de movimentos novos
  attr_reader :mcn
  # @return [Integer] movimento em tratamento
  attr_reader :mpn
  # @return [Date] data lancamento movimento em tratamento
  attr_reader :mdl
  # @return [Float] valor movimento em tratamento
  attr_reader :mvl

  # @return [Date] data contrato arrendamento
  # {:mv=>[{:dl=>#<Date: 2020-03-02>, :vl=>0.3e2},...]
  # ,:ct=>"r03000"
  # ,:dc=>#<Date: 2020-03-01>
  # ,:ano=>2020
  # ,:cnt=>0
  # ,:dl=>#<Date: 2020-03-01>}
  def initialize(ren)
    p ['Rendas', ren]
    @ren = ren
    super(ren[:ct])
  end

  # @param [Hash] con dados contrato & lista movimentos novos
  # @return [Array<Hash>] lista rendas novas para criar
  def rendas
    vars_re
    vars_mv
    r = []
    while mvl >= rru && mpn < mcn
      r << nova_re
      proximo_mv
    end
    r
  end

  # @param (see rendas)
  def vars_mv
    @mpn = 0
    @mcn = ren[:mv].count
    vars_dados_mv
  end

  # @param (see rendas)
  def vars_dados_mv
    @mdl = ren[:mv][mpn][:dl]
    @mvl = ren[:mv][mpn][:vl]
  end

  # @param (see rendas)
  def vars_re
    @ran = ren[:ano]
    @rcn = ren[:cnt]
    @rru = Float(ren[:ct][/\d+/]) / 100
  end

  # @return [Array] lista dados da renda nova
  def nova_re
    if rcn == 12
      @rcn = 1
      @ran += 1
    else
      @rcn += 1
    end
    # [rct, ran, rcn, mdl, mdl.mjd - vencimento.mjd]
    "('#{rct}',#{ran},#{rcn},'#{mdl.strftime(DF)}',#{dias})"
  end

  def dias
    mdl.mjd - (Date.new(ran, rcn, 1) >> (ren[:dc].month - 1)).mjd
  end

  # @param (see rendas)
  def proximo_mv
    @mvl -= rru
    return unless mvl < rru

    @mpn += 1
    return unless mpn < mcn

    vars_dados_mv
  end
end
